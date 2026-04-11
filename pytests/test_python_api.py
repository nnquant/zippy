import json
from datetime import datetime, timezone
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import time

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import zippy

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]


def git_output(*args: str) -> str:
    if not (WORKSPACE_ROOT / ".git").exists():
        pytest.skip("git metadata is unavailable in this workspace")

    try:
        result = subprocess.run(
            ["git", "-C", str(WORKSPACE_ROOT), *args],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError) as error:
        pytest.skip(f"git metadata is unavailable: {error}")

    return result.stdout.strip()


def reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def test_expr_factory_replaces_expr() -> None:
    factor = zippy.Expr(expression="ABS(price)", output="score")

    assert factor.expression == "ABS(price)"
    assert factor.output == "score"
    assert not hasattr(zippy, "EXPR")


def test_expression_factor_exposes_native_attributes() -> None:
    factor = zippy.ExpressionFactor(expression="ABS(price)", output="score")

    assert factor.expression == "ABS(price)"
    assert factor.output == "score"


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


def test_setup_log_returns_snapshot_and_log_path(tmp_path: Path) -> None:
    script = """
import json
import zippy

result = zippy.setup_log(
    app="py_test",
    level="info",
    log_dir=r\"\"\"%s\"\"\",
    to_console=False,
    to_file=True,
)
print(json.dumps(result))
""" % str(tmp_path)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    result = json.loads(completed.stdout.strip())

    assert result["app"] == "py_test"
    assert result["level"] == "info"
    assert result["run_id"]
    assert result["file_path"] is not None
    file_path = Path(result["file_path"])
    assert file_path.parent == tmp_path / "py_test"
    assert file_path.name.endswith(".jsonl")
    assert "_" in file_path.stem


def test_setup_log_file_contains_runtime_message(tmp_path: Path) -> None:
    script = """
import json
from pathlib import Path

import pyarrow as pa
import zippy

snapshot = zippy.setup_log(
    app="bridge_test",
    level="info",
    log_dir=r\"\"\"%s\"\"\",
    to_console=False,
    to_file=True,
)
engine = zippy.StreamTableEngine(
    name="ticks",
    input_schema=pa.schema([("price", pa.float64())]),
    target=zippy.NullPublisher(),
)
engine.start()
engine.stop()
print(json.dumps(snapshot))
""" % str(tmp_path)

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    snapshot = json.loads(completed.stdout.strip())
    file_path = Path(snapshot["file_path"])
    contents = file_path.read_text(encoding="utf-8").splitlines()
    records = [json.loads(line) for line in contents if line.strip()]

    assert any(
        record.get("component") == "runtime"
        and record.get("event") == "start"
        and record.get("message")
        for record in records
    )
    assert any(
        record.get("component") == "python_bridge"
        and record.get("event") == "stop"
        and record.get("message") == "python runtime bridge stopped"
        for record in records
    )


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
            zippy.Expr(expression="price + ema_2", output="price_plus_ema"),
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


def test_reactive_engine_expr_supports_ts_nodes() -> None:
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
            pa.field("score", pa.float64(), nullable=True),
        ]
    )

    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.Expr(
                expression="TS_DIFF(price, 2) / TS_STD(TS_DIFF(price, 2), 3)",
                output="score",
            ),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write(
        {
            "symbol": ["A", "A", "A", "A", "A", "A"],
            "price": [10.0, 11.0, 13.0, 16.0, 20.0, 25.0],
        }
    )
    engine.flush()

    received = subscriber.recv()

    assert received.column(2).to_pylist() == [
        None,
        None,
        None,
        None,
        pytest.approx(4.286607049870562),
        pytest.approx(5.5113519212621505),
    ]

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
            factors=[zippy.Expr(expression="missing + 1.0", output="score")],
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
            factors=[zippy.Expr(expression="sqrt(price)", output="score")],
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
            zippy.Expr(expression="price * volume", output="turnover_input"),
        ],
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
            zippy.AGG_SUM(column="turnover_input", output="turnover"),
        ],
        post_factors=[
            zippy.Expr(expression="close / open - 1.0", output="ret_1m"),
            zippy.Expr(expression="turnover / volume", output="vwap_1m"),
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
                zippy.Expr(expression="price * 2.0", output="bad"),
            ],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_pre_factors_reject_ts_expr_nodes() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    with pytest.raises(
        ValueError,
        match="stateful TS_\\* functions are only supported inside ReactiveStateEngine",
    ):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            pre_factors=[
                zippy.Expr(expression="TS_DIFF(price, 2)", output="bad"),
            ],
            factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            target=zippy.NullPublisher(),
        )


def test_timeseries_engine_post_factors_reject_ts_expr_nodes() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )

    with pytest.raises(
        ValueError,
        match="stateful TS_\\* functions are only supported inside ReactiveStateEngine",
    ):
        zippy.TimeSeriesEngine(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            post_factors=[
                zippy.Expr(expression="TS_STD(sum_price, 2)", output="bad"),
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
            zippy.Expr(expression="LOG(price)", output="log_price"),
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
            zippy.Expr(expression="price * volume", output="turnover_input"),
        ],
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
            zippy.AGG_SUM(column="turnover_input", output="turnover"),
        ],
        post_factors=[
            zippy.Expr(expression="close / open - 1.0", output="ret_1m"),
            zippy.Expr(expression="turnover / volume", output="vwap_1m"),
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
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [11.0]}))
    engine.stop()

    parquet_files = list(tmp_path.rglob("*.parquet"))
    input_files = [path for path in parquet_files if "input" in path.parts]
    output_files = [path for path in parquet_files if "output" in path.parts]

    assert input_files
    assert output_files
    assert pq.read_schema(input_files[0]).names == ["symbol", "price"]
    assert pq.read_schema(output_files[0]).names == ["symbol", "price", "ema_2"]
    assert len(input_files) == 1
    assert len(output_files) == 1
    assert pq.read_table(input_files[0]).num_rows == 2
    assert pq.read_table(output_files[0]).num_rows == 2


def test_parquet_sink_flush_interval_flushes_small_batches(tmp_path: Path) -> None:
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
            rotation="none",
            write_input=True,
            write_output=False,
            rows_per_batch=10_000,
            flush_interval_ms=50,
        ),
    )

    engine.start()
    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    time.sleep(0.12)
    engine.stop()

    input_files = list((tmp_path / "input").rglob("*.parquet"))

    assert len(input_files) == 1
    assert pq.read_table(input_files[0]).num_rows == 1


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
            rows_per_batch=4,
            flush_interval_ms=250,
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
            rows_per_batch=4,
            flush_interval_ms=250,
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
        "filtered_rows_total": 0,
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
        "rows_per_batch": 4,
        "flush_interval_ms": 250,
    }

    engine.start()
    assert engine.status() == "running"

    engine.write(pl.DataFrame({"symbol": ["A"], "price": [10.0]}))
    engine.flush()

    metrics = engine.metrics()
    assert metrics["processed_batches_total"] == 1
    assert metrics["processed_rows_total"] == 1
    assert metrics["output_batches_total"] == 1
    assert metrics["filtered_rows_total"] == 0
    assert metrics["queue_depth"] == 0

    engine.stop()
    assert engine.status() == "stopped"
    assert engine.metrics()["processed_batches_total"] == 1


def test_reactive_engine_id_filter_filters_rows_and_updates_config_and_metrics() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="reactive_filter",
        input_schema=schema,
        id_column="symbol",
        factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
        id_filter=["A"],
    )

    config = engine.config()
    assert config["id_filter"] == ["A"]

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    engine.write({"symbol": ["A", "B"], "price": [10.0, 99.0]})
    received = subscriber.recv()

    assert received.column_names == ["symbol", "price", "price_x2"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(1).to_pylist() == [10.0]
    assert received.column(2).to_pylist() == [20.0]

    engine.stop()
    metrics = engine.metrics()
    assert metrics["filtered_rows_total"] == 1
    assert metrics["late_rows_total"] == 0
    subscriber.close()


def test_reactive_engine_rejects_empty_id_filter() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="id_filter must not be empty"):
        zippy.ReactiveStateEngine(
            name="reactive_filter",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
            target=zippy.NullPublisher(),
            id_filter=[],
        )


def test_reactive_engine_rejects_non_string_id_filter_elements() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="id_filter must be a sequence of strings"):
        zippy.ReactiveStateEngine(
            name="reactive_filter",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
            target=zippy.NullPublisher(),
            id_filter=["A", 1],
        )


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
    assert config["parquet_sink"]["rows_per_batch"] == 8192
    assert config["parquet_sink"]["flush_interval_ms"] == 1000

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


def test_timeseries_engine_id_filter_filters_rows_and_updates_config_and_metrics() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.TimeSeriesEngine(
        name="bar_filter",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
        id_filter=["A"],
    )

    config = engine.config()
    assert config["id_filter"] == ["A"]

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    engine.write(
        {
            "symbol": ["A", "B"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 99.0],
        }
    )
    engine.flush()
    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "window_end", "close"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(3).to_pylist() == [10.0]

    engine.stop()
    metrics = engine.metrics()
    assert metrics["filtered_rows_total"] == 1
    assert metrics["late_rows_total"] == 0
    subscriber.close()


def test_timeseries_engine_rejects_empty_id_filter() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="id_filter must not be empty"):
        zippy.TimeSeriesEngine(
            name="bar_filter",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_LAST(column="price", output="close")],
            target=zippy.NullPublisher(),
            id_filter=[],
        )


def test_timeseries_engine_rejects_non_string_id_filter_elements() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="id_filter must be a sequence of strings"):
        zippy.TimeSeriesEngine(
            name="bar_filter",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_LAST(column="price", output="close")],
            target=zippy.NullPublisher(),
            id_filter=["A", 1],
        )


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


def test_stream_table_engine_exposes_input_schema_as_output_schema() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    engine = zippy.StreamTableEngine(
        name="ticks",
        input_schema=schema,
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == schema


def test_stream_table_engine_supports_source_target_and_sink(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    source = zippy.ZmqSource(
        endpoint="tcp://127.0.0.1:9999",
        expected_schema=schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    engine = zippy.StreamTableEngine(
        name="ticks",
        input_schema=schema,
        source=source,
        target=zippy.NullPublisher(),
        sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=True,
            write_output=True,
            rows_per_batch=16,
            flush_interval_ms=500,
        ),
    )

    config = engine.config()

    assert config["engine_type"] == "stream_table"
    assert config["source_linked"] is True
    assert config["has_sink"] is True
    assert config["sink"] == {
        "path": str(tmp_path),
        "rotation": "none",
        "write_input": True,
        "write_output": True,
        "rows_per_batch": 16,
        "flush_interval_ms": 500,
    }
    assert "parquet_sink" not in config


def test_stream_table_engine_rejects_invalid_sink_type() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
        ]
    )

    with pytest.raises(TypeError, match="sink must be zippy.ParquetSink"):
        zippy.StreamTableEngine(
            name="ticks",
            input_schema=schema,
            target=zippy.NullPublisher(),
            sink=zippy.NullPublisher(),
        )


def test_stream_table_engine_passthrough_archives_output_and_publishes_remote_stream(
    tmp_path: Path,
) -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    first_dt = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
    second_dt = datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc)
    relay_port = reserve_tcp_port()
    relay_endpoint = f"tcp://127.0.0.1:{relay_port}"

    stream_target = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="tick_table",
        schema=schema,
    )
    relay_source = zippy.ZmqSource(
        endpoint=stream_target.last_endpoint(),
        expected_schema=schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    relay = zippy.StreamTableEngine(
        name="tick_table_relay",
        source=relay_source,
        input_schema=schema,
        target=zippy.ZmqPublisher(endpoint=relay_endpoint),
    )
    engine = zippy.StreamTableEngine(
        name="tick_table",
        input_schema=schema,
        target=stream_target,
        sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=False,
            write_output=True,
        ),
    )

    relay.start()
    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=relay_endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    engine.write(
        pl.DataFrame(
            {
                "symbol": ["IF2606", "IH2606"],
                "dt": [first_dt, second_dt],
                "price": [3898.2, 2675.4],
                "volume": [12.0, 8.0],
            }
        )
    )
    engine.flush()

    received = subscriber.recv()

    assert received.schema == schema
    assert received.column(0).to_pylist() == ["IF2606", "IH2606"]
    assert received.column(1).to_pylist() == [first_dt, second_dt]
    assert received.column(2).to_pylist() == [3898.2, 2675.4]
    assert received.column(3).to_pylist() == [12.0, 8.0]

    engine.stop()
    relay.stop()
    subscriber.close()

    parquet_files = list(tmp_path.rglob("*.parquet"))
    output_files = [path for path in parquet_files if "output" in path.parts]

    assert output_files

    archived = pq.read_table(output_files[0])
    assert archived.schema == schema
    assert archived.column("symbol").to_pylist() == ["IF2606", "IH2606"]
    assert archived.column("dt").to_pylist() == [first_dt, second_dt]
    assert archived.column("price").to_pylist() == [3898.2, 2675.4]
    assert archived.column("volume").to_pylist() == [12.0, 8.0]


def test_stream_table_engine_can_drive_timeseries_downstream_pipeline() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    source = zippy.StreamTableEngine(
        name="tick_table",
        input_schema=tick_schema,
        target=zippy.NullPublisher(),
    )
    bars = zippy.TimeSeriesEngine(
        name="bar_1m",
        source=source,
        input_schema=tick_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.AGG_FIRST(column="price", output="open"),
            zippy.AGG_LAST(column="price", output="close"),
            zippy.AGG_SUM(column="volume", output="volume"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
    bucket_end = datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc)

    bars.start()
    source.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    source.write(
        {
            "symbol": ["A", "A"],
            "dt": [bucket_start, datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc)],
            "price": [10.0, 11.0],
            "volume": [100.0, 120.0],
        }
    )
    source.flush()
    bars.flush()

    received = subscriber.recv()

    assert received.column_names == [
        "symbol",
        "window_start",
        "window_end",
        "open",
        "close",
        "volume",
    ]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(1).to_pylist() == [bucket_start]
    assert received.column(2).to_pylist() == [bucket_end]
    assert received.column(3).to_pylist() == [10.0]
    assert received.column(4).to_pylist() == [11.0]
    assert received.column(5).to_pylist() == [220.0]

    source.stop()
    bars.stop()
    subscriber.close()


def test_stream_table_engine_can_publish_to_master_bus(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    control_endpoint = str(tmp_path / "zippy-master.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    server.start()

    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master.register_process("writer")
    writer_master.register_stream("ticks", schema, 64)

    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_master.register_process("reader")

    engine = zippy.StreamTableEngine(
        name="ticks",
        input_schema=schema,
        target=zippy.BusStreamTarget(stream_name="ticks", master=writer_master),
    )
    reader = reader_master.read_from("ticks")

    try:
        engine.start()
        engine.write(
            pl.DataFrame(
                {
                    "instrument_id": ["IF2606", "IH2606"],
                    "last_price": [3898.2, 2675.4],
                }
            )
        )
        received = reader.read(1_000)

        assert received.schema == schema
        assert received.column("instrument_id").to_pylist() == ["IF2606", "IH2606"]
        assert received.column("last_price").to_pylist() == [3898.2, 2675.4]
    finally:
        reader.close()
        engine.stop()
        server.stop()
        server.join()


def test_master_server_start_raises_when_socket_is_already_active(tmp_path: Path) -> None:
    control_endpoint = str(tmp_path / "zippy-master.sock")
    first_server = zippy.MasterServer(control_endpoint=control_endpoint)
    second_server = zippy.MasterServer(control_endpoint=control_endpoint)
    first_server.start()

    try:
        with pytest.raises(RuntimeError, match="control endpoint socket is already active"):
            second_server.start()
    finally:
        first_server.stop()
        first_server.join()


def test_master_server_start_waits_for_configured_ready_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_endpoint = str(tmp_path / "zippy-master.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    monkeypatch.setenv("ZIPPY_MASTER_TEST_PAUSE_BEFORE_READY_MS", "1200")

    try:
        server.start(3.0)
        assert Path(control_endpoint).exists()
    finally:
        server.stop()
        server.join()


def test_master_server_start_expands_user_path_and_creates_parent_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    control_endpoint = "~/.zippy/master.sock"
    expected_socket_path = tmp_path / ".zippy" / "master.sock"
    monkeypatch.setenv("HOME", str(tmp_path))
    server = zippy.MasterServer(control_endpoint=control_endpoint)

    try:
        server.start()
        assert expected_socket_path.exists()
    finally:
        server.stop()
        server.join()


def test_run_master_daemon_expands_user_path_and_creates_parent_dir(tmp_path: Path) -> None:
    socket_path = tmp_path / ".zippy" / "master.sock"
    script = "import zippy; zippy.run_master_daemon('~/.zippy/master.sock')"
    env = os.environ.copy()
    env["HOME"] = str(tmp_path)
    process = subprocess.Popen(
        [sys.executable, "-c", script],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )

    try:
        deadline = time.time() + 10.0
        while time.time() < deadline:
            if socket_path.exists():
                break
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                raise AssertionError(
                    "run_master_daemon exited before creating expanded socket "
                    f"stdout=[{stdout}] stderr=[{stderr}]"
                )
            time.sleep(0.05)
        else:
            stdout, stderr = process.communicate(timeout=5)
            raise AssertionError(
                "run_master_daemon did not create expanded socket before timeout "
                f"stdout=[{stdout}] stderr=[{stderr}]"
            )

        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=10)

        assert process.returncode == 0, stdout + stderr
        assert socket_path.parent.is_dir()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)


def test_reactive_engine_can_consume_master_bus_stream(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    control_endpoint = str(tmp_path / "zippy-master.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    server.start()

    writer_master = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_master.register_process("writer")
    writer_master.register_stream("ticks", schema, 64)
    writer = writer_master.write_to("ticks")

    reader_master = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_master.register_process("reactive_reader")

    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    engine = zippy.ReactiveStateEngine(
        name="reactive_bus",
        source=zippy.BusStreamSource(
            stream_name="ticks",
            expected_schema=schema,
            master=reader_master,
            mode=zippy.SourceMode.PIPELINE,
        ),
        input_schema=schema,
        id_column="instrument_id",
        factors=[
            zippy.Expr(expression="last_price * 2.0", output="price_x2"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    try:
        engine.start()
        time.sleep(0.3)
        writer.write(
            pl.DataFrame(
                {
                    "instrument_id": ["IF2606"],
                    "last_price": [10.0],
                }
            )
        )

        deadline = time.time() + 2.0
        while True:
            try:
                received = subscriber.recv()
                break
            except RuntimeError as error:
                if (
                    "Resource temporarily unavailable" not in str(error)
                    or time.time() >= deadline
                ):
                    raise
                time.sleep(0.05)
        assert received.column("instrument_id").to_pylist() == ["IF2606"]
        assert received.column("last_price").to_pylist() == [10.0]
        assert received.column("price_x2").to_pylist() == [20.0]
    finally:
        writer.close()
        engine.stop()
        subscriber.close()
        server.stop()
        server.join()


def test_cross_sectional_engine_accepts_duration_trigger_interval_and_exposes_output_schema() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            pa.field("ret_rank", pa.float64(), nullable=True),
            pa.field("ret_z", pa.float64(), nullable=True),
            pa.field("ret_dm", pa.float64(), nullable=True),
        ]
    )

    rank = zippy.CS_RANK(column="ret_1m", output="ret_rank")
    zscore = zippy.CS_ZSCORE(column="ret_1m", output="ret_z")
    demean = zippy.CS_DEMEAN(column="ret_1m", output="ret_dm")

    assert isinstance(rank, zippy.CSRankSpec)
    assert isinstance(zscore, zippy.CSZscoreSpec)
    assert isinstance(demean, zippy.CSDemeanSpec)

    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[rank, zscore, demean],
        target=zippy.NullPublisher(),
    )

    assert engine.output_schema() == expected_output_schema

    engine.start()

    assert engine.output_schema() == expected_output_schema

    engine.stop()


def test_cross_sectional_engine_emits_bucketed_output_over_zmq() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[
            zippy.CS_RANK(column="ret_1m", output="ret_rank"),
            zippy.CS_ZSCORE(column="ret_1m", output="ret_z"),
            zippy.CS_DEMEAN(column="ret_1m", output="ret_dm"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    engine.write(
        {
            "symbol": ["B", "A", "C"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 2, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 3, tzinfo=timezone.utc),
            ],
            "ret_1m": [2.0, 1.0, 3.0],
        }
    )
    engine.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "dt", "ret_rank", "ret_z", "ret_dm"]
    assert received.column(0).to_pylist() == ["A", "B", "C"]
    assert received.column(1).to_pylist() == [bucket_start, bucket_start, bucket_start]
    assert received.column(2).to_pylist() == pytest.approx([1.0, 2.0, 3.0])
    assert received.column(3).to_pylist() == pytest.approx(
        [-1.224744871391589, 0.0, 1.224744871391589]
    )
    assert received.column(4).to_pylist() == pytest.approx([-1.0, 0.0, 1.0])

    engine.stop()
    subscriber.close()


def test_cross_sectional_engine_rejects_unsupported_late_data_policy() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    with pytest.raises(ValueError, match="late_data_policy"):
        zippy.CrossSectionalEngine(
            name="cs_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            trigger_interval=zippy.Duration.minutes(1),
            late_data_policy=zippy.LateDataPolicy.DROP_WITH_METRIC,
            factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
            target=zippy.NullPublisher(),
        )


def test_cross_sectional_engine_rejects_non_cross_sectional_factor_specs() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    with pytest.raises(TypeError, match="CSRankSpec|CSZscoreSpec|CSDemeanSpec"):
        zippy.CrossSectionalEngine(
            name="cs_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            trigger_interval=zippy.Duration.minutes(1),
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_FIRST(column="ret_1m", output="bad")],
            target=zippy.NullPublisher(),
        )


def test_cross_sectional_engine_rejects_reactive_source() -> None:
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

    with pytest.raises(TypeError, match="source must be TimeSeriesEngine"):
        zippy.CrossSectionalEngine(
            name="cs_1m",
            source=reactive,
            input_schema=reactive.output_schema(),
            id_column="symbol",
            dt_column="dt",
            trigger_interval=zippy.Duration.minutes(1),
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.CS_RANK(column="price", output="price_rank")],
            target=zippy.NullPublisher(),
        )


def test_cross_sectional_engine_accepts_timeseries_source_pipeline() -> None:
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
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)

    cs = zippy.CrossSectionalEngine(
        name="cs_1m",
        source=bars,
        input_schema=bars.output_schema(),
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="open", output="open_rank")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    reactive.start()
    bars.start()
    cs.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    reactive.write(
        {
            "symbol": ["A", "A", "B", "B"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 31, 0, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0, 20.0, 21.0],
        }
    )
    reactive.flush()
    bars.flush()
    cs.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "open_rank"]
    assert received.column(0).to_pylist() == ["A", "B"]
    assert received.column(1).to_pylist() == [bucket_start, bucket_start]
    assert received.column(2).to_pylist() == pytest.approx([1.0, 2.0])

    reactive.stop()
    bars.stop()
    cs.stop()
    subscriber.close()


def test_source_mode_constants_and_zmq_source_are_exposed() -> None:
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    assert repr(zippy.SourceMode.PIPELINE) == "SourceMode.PIPELINE"
    assert repr(zippy.SourceMode.CONSUMER) == "SourceMode.CONSUMER"

    source = zippy.ZmqSource(
        endpoint="tcp://127.0.0.1:7101",
        expected_schema=schema,
        mode=zippy.SourceMode.PIPELINE,
    )

    assert isinstance(source, zippy.ZmqSource)


def test_zmq_source_pipeline_can_drive_cross_sectional_engine() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("window_start", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="bars",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.CrossSectionalEngine(
        name="cs_remote",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)

    upstream.publish(
        {
            "symbol": ["A", "B"],
            "window_start": [bucket_start, bucket_start],
            "ret_1m": [0.1, 0.2],
        }
    )
    upstream.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "ret_rank"]
    assert received.column(0).to_pylist() == ["A", "B"]
    assert received.column(1).to_pylist() == [bucket_start, bucket_start]
    assert received.column(2).to_pylist() == pytest.approx([1.0, 2.0])

    engine.stop()
    upstream.stop()
    subscriber.close()


def test_timeseries_engine_accepts_zmq_stream_target_for_remote_pipeline() -> None:
    tick_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    schema_probe = zippy.TimeSeriesEngine(
        name="bars_probe",
        input_schema=tick_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.NullPublisher(),
    )
    bar_schema = schema_probe.output_schema()

    stream_target = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="bars",
        schema=bar_schema,
    )
    source = zippy.ZmqSource(
        endpoint=stream_target.last_endpoint(),
        expected_schema=bar_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    upstream = zippy.TimeSeriesEngine(
        name="bars_upstream",
        input_schema=tick_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=stream_target,
    )
    downstream = zippy.CrossSectionalEngine(
        name="cs_remote",
        source=source,
        input_schema=bar_schema,
        id_column="symbol",
        dt_column="window_start",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="close", output="close_rank")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    downstream.start()
    upstream.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)

    upstream.write(
        {
            "symbol": ["A", "B"],
            "dt": [bucket_start, bucket_start],
            "price": [10.0, 20.0],
        }
    )
    upstream.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "close_rank"]
    assert received.column(0).to_pylist() == ["A", "B"]
    assert received.column(1).to_pylist() == [bucket_start, bucket_start]
    assert received.column(2).to_pylist() == pytest.approx([1.0, 2.0])

    upstream.stop()
    downstream.stop()
    subscriber.close()


def test_timeseries_engine_filters_remote_zmq_source_rows_by_id_filter() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="ticks",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.TimeSeriesEngine(
        name="bar_remote_filter",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
        id_filter=["A"],
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    upstream.publish(
        {
            "symbol": ["A", "B"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 99.0],
        }
    )
    upstream.flush()

    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "window_end", "close"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(3).to_pylist() == [10.0]

    engine.stop()
    assert engine.metrics()["filtered_rows_total"] == 1
    upstream.stop()
    subscriber.close()


def test_zmq_source_consumer_mode_ignores_upstream_flush_until_local_flush() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="ticks",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.CONSUMER,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.TimeSeriesEngine(
        name="bar_remote",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window=zippy.Duration.minutes(1),
        window_type=zippy.WindowType.TUMBLING,
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.AGG_LAST(column="price", output="close")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=200)
    time.sleep(0.1)

    upstream.publish(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
        }
    )
    upstream.flush()

    with pytest.raises(RuntimeError):
        subscriber.recv()

    engine.flush()
    received = subscriber.recv()

    assert received.column_names == ["symbol", "window_start", "window_end", "close"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(3).to_pylist() == [11.0]

    engine.stop()
    upstream.stop()
    subscriber.close()


def test_timeseries_engine_rejects_remote_source_schema_mismatch() -> None:
    source_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("close", pa.float64()),
        ]
    )

    source = zippy.ZmqSource(
        endpoint="tcp://127.0.0.1:7102",
        expected_schema=source_schema,
        mode=zippy.SourceMode.PIPELINE,
    )

    with pytest.raises(ValueError, match="source output schema must match downstream input_schema"):
        zippy.TimeSeriesEngine(
            name="bar_remote",
            source=source,
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window=zippy.Duration.minutes(1),
            window_type=zippy.WindowType.TUMBLING,
            late_data_policy=zippy.LateDataPolicy.REJECT,
            factors=[zippy.AGG_LAST(column="close", output="last_close")],
            target=zippy.NullPublisher(),
        )


def test_reactive_engine_processes_remote_zmq_source() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="ticks",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="reactive_remote",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.Expr(expression="price * 2.0", output="price_x2"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    upstream.publish(
        {
            "symbol": ["A", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 11.0],
        }
    )

    received = subscriber.recv()

    assert received.column_names == ["symbol", "dt", "price", "price_x2"]
    assert received.column(0).to_pylist() == ["A", "A"]
    assert received.column(2).to_pylist() == [10.0, 11.0]
    assert received.column(3).to_pylist() == [20.0, 22.0]

    engine.stop()
    upstream.stop()
    subscriber.close()


def test_reactive_engine_filters_remote_zmq_source_rows_by_id_filter() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
        ]
    )

    upstream = zippy.ZmqStreamPublisher(
        endpoint="tcp://127.0.0.1:*",
        stream_name="ticks",
        schema=input_schema,
    )
    source = zippy.ZmqSource(
        endpoint=upstream.last_endpoint(),
        expected_schema=input_schema,
        mode=zippy.SourceMode.PIPELINE,
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"
    engine = zippy.ReactiveStateEngine(
        name="reactive_remote_filter",
        source=source,
        input_schema=input_schema,
        id_column="symbol",
        id_filter=["A"],
        factors=[
            zippy.Expr(expression="price * 2.0", output="price_x2"),
        ],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    engine.start()
    subscriber = zippy.ZmqSubscriber(endpoint=endpoint, timeout_ms=1_000)
    time.sleep(0.1)

    upstream.publish(
        {
            "symbol": ["A", "B"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "price": [10.0, 99.0],
        }
    )

    received = subscriber.recv()

    assert received.column_names == ["symbol", "dt", "price", "price_x2"]
    assert received.column(0).to_pylist() == ["A"]
    assert received.column(2).to_pylist() == [10.0]
    assert received.column(3).to_pylist() == [20.0]

    engine.stop()
    assert engine.metrics()["filtered_rows_total"] == 1
    upstream.stop()
    subscriber.close()


def test_cross_sectional_engine_archives_output_parquet(tmp_path: Path) -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    bucket_start = datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc)

    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.NullPublisher(),
        parquet_sink=zippy.ParquetSink(
            path=str(tmp_path),
            rotation="none",
            write_input=False,
            write_output=True,
        ),
    )

    engine.start()
    engine.write(
        {
            "symbol": ["B", "A"],
            "dt": [
                datetime(2026, 4, 2, 9, 30, 2, tzinfo=timezone.utc),
                datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
            ],
            "ret_1m": [2.0, 1.0],
        }
    )
    engine.stop()

    output_files = list((tmp_path / "output").rglob("*.parquet"))

    assert output_files
    output_table = pq.read_table(output_files[0])
    assert output_table.column_names == ["symbol", "dt", "ret_rank"]
    assert output_table.column("symbol").to_pylist() == ["A", "B"]
    assert output_table.column("dt").to_pylist() == [bucket_start, bucket_start]
    assert output_table.column("ret_rank").to_pylist() == pytest.approx([1.0, 2.0])


def test_cross_sectional_engine_start_can_retry_after_publisher_failure() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("ret_1m", pa.float64()),
        ]
    )
    port = reserve_tcp_port()
    endpoint = f"tcp://127.0.0.1:{port}"

    blocker = zippy.ReactiveStateEngine(
        name="blocker",
        input_schema=pa.schema([("symbol", pa.string()), ("price", pa.float64())]),
        id_column="symbol",
        factors=[zippy.TS_EMA(column="price", span=2, output="ema_2")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )
    engine = zippy.CrossSectionalEngine(
        name="cs_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        trigger_interval=zippy.Duration.minutes(1),
        late_data_policy=zippy.LateDataPolicy.REJECT,
        factors=[zippy.CS_RANK(column="ret_1m", output="ret_rank")],
        target=zippy.ZmqPublisher(endpoint=endpoint),
    )

    blocker.start()

    with pytest.raises(RuntimeError):
        engine.start()

    blocker.stop()
    engine.start()
    engine.stop()


def start_master_server(tmp_path: Path) -> tuple[zippy.MasterServer, str]:
    control_endpoint = str(tmp_path / "zippy-master.sock")
    server = zippy.MasterServer(control_endpoint=control_endpoint)
    server.start()
    return server, control_endpoint


def test_master_client_roundtrips_batches_through_master_bus_direct_reader(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    writer_client = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_client.register_process("writer")
    writer_client.register_stream("ticks", tick_schema, 64)
    writer = writer_client.write_to("ticks")

    reader_client = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_client.register_process("reader")
    reader = reader_client.read_from("ticks")

    writer.write({"instrument_id": ["IF2606"], "last_price": [4102.5]})
    batch = reader.read(timeout_ms=1000)

    assert batch.schema == tick_schema
    assert batch.column("instrument_id").to_pylist() == ["IF2606"]
    assert batch.column("last_price").to_pylist() == [4102.5]

    reader.close()
    writer.close()
    server.stop()


def test_master_bus_writer_close_releases_stream_for_restart(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )

    first_client = zippy.MasterClient(control_endpoint=control_endpoint)
    first_client.register_process("writer_1")
    first_client.register_stream("ticks", tick_schema, 64)
    first_writer = first_client.write_to("ticks")
    first_writer.close()

    second_client = zippy.MasterClient(control_endpoint=control_endpoint)
    second_client.register_process("writer_2")
    second_client.register_stream("ticks", tick_schema, 64)
    second_writer = second_client.write_to("ticks")

    second_writer.write({"instrument_id": ["IF2606"], "last_price": [4102.5]})
    second_writer.close()
    server.stop()


def test_stream_table_engine_can_publish_to_master_bus_direct_reader(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    writer_client = zippy.MasterClient(control_endpoint=control_endpoint)
    writer_client.register_process("stream_table_writer")
    writer_client.register_stream("openctp_ticks", tick_schema, 64)

    reader_client = zippy.MasterClient(control_endpoint=control_endpoint)
    reader_client.register_process("stream_table_reader")
    reader = reader_client.read_from("openctp_ticks")

    engine = zippy.StreamTableEngine(
        name="ticks",
        input_schema=tick_schema,
        target=zippy.BusStreamTarget(
            stream_name="openctp_ticks",
            master=writer_client,
        ),
    )

    engine.start()
    engine.write(
        {
            "instrument_id": ["IF2606"],
            "dt": [datetime(2026, 4, 10, 9, 30, 0, tzinfo=timezone.utc)],
            "last_price": [4102.5],
        }
    )
    received = reader.read(timeout_ms=1000)
    engine.stop()

    assert received.schema == tick_schema
    assert received.column("instrument_id").to_pylist() == ["IF2606"]
    assert received.column("last_price").to_pylist() == [4102.5]

    reader.close()
    server.stop()


def test_master_client_lists_streams(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema([("instrument_id", pa.string())])

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream("openctp_ticks", tick_schema, 64)

    streams = client.list_streams()

    assert len(streams) == 1
    assert streams[0]["stream_name"] == "openctp_ticks"
    assert streams[0]["ring_capacity"] == 64
    assert streams[0]["writer_process_id"] is None
    assert streams[0]["reader_count"] == 0
    assert streams[0]["status"] == "registered"

    server.stop()


def test_master_client_gets_stream(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema([("instrument_id", pa.string())])

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    client.register_process("writer")
    client.register_stream("openctp_ticks", tick_schema, 64)

    stream = client.get_stream("openctp_ticks")

    assert stream["stream_name"] == "openctp_ticks"
    assert stream["ring_capacity"] == 64
    assert stream["writer_process_id"] is None
    assert stream["reader_count"] == 0
    assert stream["status"] == "registered"

    server.stop()


def test_master_client_heartbeat_keeps_registered_process_alive(tmp_path: Path) -> None:
    server, control_endpoint = start_master_server(tmp_path)

    client = zippy.MasterClient(control_endpoint=control_endpoint)
    process_id = client.register_process("worker_a")
    client.heartbeat()

    assert process_id.startswith("proc_")

    server.stop()


def test_reactive_engine_can_consume_master_bus_stream_and_publish_to_bus(
    tmp_path: Path,
) -> None:
    server, control_endpoint = start_master_server(tmp_path)
    tick_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    factor_schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
            ("price_x2", pa.float64()),
        ]
    )

    input_client = zippy.MasterClient(control_endpoint=control_endpoint)
    input_client.register_process("tick_writer")
    input_client.register_stream("openctp_ticks", tick_schema, 64)
    writer = input_client.write_to("openctp_ticks")

    factor_client = zippy.MasterClient(control_endpoint=control_endpoint)
    factor_client.register_process("reactive_engine")
    factor_client.register_stream("openctp_factor_ticks", factor_schema, 64)

    output_client = zippy.MasterClient(control_endpoint=control_endpoint)
    output_client.register_process("factor_reader")
    output_reader = output_client.read_from("openctp_factor_ticks")

    engine = zippy.ReactiveStateEngine(
        name="reactive_bus",
        input_schema=tick_schema,
        id_column="instrument_id",
        factors=[zippy.Expr(expression="last_price * 2.0", output="price_x2")],
        source=zippy.BusStreamSource(
            stream_name="openctp_ticks",
            expected_schema=tick_schema,
            master=factor_client,
            mode=zippy.SourceMode.PIPELINE,
        ),
        target=zippy.BusStreamTarget(
            stream_name="openctp_factor_ticks",
            master=factor_client,
        ),
    )

    engine.start()
    writer.write({"instrument_id": ["IF2606"], "last_price": [4102.5]})
    received = output_reader.read(timeout_ms=1000)
    engine.stop()

    assert received.schema == factor_schema
    assert received.column("instrument_id").to_pylist() == ["IF2606"]
    assert received.column("price_x2").to_pylist() == [8205.0]

    output_reader.close()
    writer.close()
    server.stop()
