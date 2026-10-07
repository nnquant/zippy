"""Native factor semantics, replay and package contracts after retiring legacy bindings."""

from collections.abc import Callable
from typing import Any
import json
import tomllib
from datetime import datetime, timezone
import os
from pathlib import Path
import signal
import subprocess
import sys
import threading
import time
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import zippy

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]


def construct_for_rejection(factory: Callable[..., object], *args: Any, **kwargs: Any) -> object:
    """Send intentionally invalid runtime inputs past the static API contract."""
    return factory(*args, **kwargs)


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


def test_expr_factory_replaces_expr() -> None:
    factor = zippy.Expr(expression="ABS(price)", output="score")

    assert factor.expression == "ABS(price)"
    assert factor.output == "score"
    assert not hasattr(zippy, "EXPR")


def test_expression_factor_exposes_native_attributes() -> None:
    factor = zippy.ExpressionFactor(expression="ABS(price)", output="score")

    assert factor.expression == "ABS(price)"
    assert factor.output == "score"


def test_parquet_replay_source_emits_parquet_batches(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("last_price", pa.float64()),
        ]
    )
    table = pa.table(
        {
            "instrument_id": ["IF2606", "IH2606", "IC2606"],
            "last_price": [4102.5, 2711.0, 6123.5],
        },
        schema=schema,
    )
    parquet_path = tmp_path / "ticks.parquet"
    pq.write_table(table, parquet_path)

    class CapturingSink:
        def __init__(self) -> None:
            self.hello: list[tuple[str, int]] = []
            self.tables: list[pa.Table] = []
            self.flushed = False
            self.stopped = False
            self.flushed_event = threading.Event()

        def emit_hello(self, stream_name: str, protocol_version: int = 1) -> None:
            self.hello.append((stream_name, protocol_version))

        def emit_data(self, value) -> None:
            self.tables.append(value)

        def emit_flush(self) -> None:
            self.flushed = True
            self.flushed_event.set()

        def emit_stop(self) -> None:
            self.stopped = True

        def emit_error(self, reason: str) -> None:
            raise AssertionError(reason)

    source = zippy.ParquetReplaySource(parquet_path, batch_size=2)
    sink = CapturingSink()

    handle = source._zippy_start(sink)
    assert sink.flushed_event.wait(timeout=2.0)
    handle.stop()
    handle.join()

    assert source._zippy_output_schema() == schema
    assert source._zippy_source_name() == "parquet_replay"
    assert source._zippy_source_type() == "parquet_replay"
    assert sink.hello == [("parquet_replay", 1)]
    assert [item.num_rows for item in sink.tables] == [2, 1]
    assert pa.concat_tables(sink.tables) == table
    assert sink.flushed is True
    assert sink.stopped is True


def test_parquet_replay_engine_table_callback_matches_row_callback_order(tmp_path: Path) -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("last_price", pa.float64()),
        ]
    )
    table = pa.table(
        {
            "instrument_id": ["IF2606", "IH2606", "IC2606"],
            "dt": [1777017600000000000, 1777017600000001000, 1777017600000002000],
            "last_price": [3898.0, 2675.0, 6123.0],
        },
        schema=schema,
    )
    parquet_path = tmp_path / "ticks.parquet"
    pq.write_table(table, parquet_path)
    row_received: list[dict[str, object]] = []
    table_received: list[pa.Table] = []

    zippy.ParquetReplayEngine(
        parquet_path,
        callback=lambda row: row_received.append(row.to_dict()),
        schema=schema,
        batch_size=2,
    ).run()
    table_engine = zippy.ParquetReplayEngine(
        parquet_path,
        callback=table_received.append,
        table_callback=True,
        schema=schema,
        batch_size=2,
    ).run()

    expected_rows = table.set_column(1, "dt", table.column("dt").cast(pa.int64())).to_pylist()
    assert row_received == expected_rows
    assert [batch.num_rows for batch in table_received] == [2, 1]
    assert pa.concat_tables(table_received).equals(table, check_metadata=True)
    assert table_engine.metrics()["callback_mode"] == "table"
    assert table_engine.metrics()["batches_delivered_total"] == 2
    assert table_engine.metrics()["rows_delivered_total"] == 3


@pytest.mark.parametrize("table_callback", [False, True])
def test_parquet_replay_engine_callback_errors_propagate(
    tmp_path: Path,
    table_callback: bool,
) -> None:
    schema = pa.schema([("seq", pa.int64())])
    parquet_path = tmp_path / "ticks.parquet"
    pq.write_table(pa.table({"seq": [1, 2]}, schema=schema), parquet_path)

    def on_replay(_payload) -> None:
        raise ValueError("replay callback failed")

    with pytest.raises(RuntimeError, match="callback replay failed"):
        zippy.ParquetReplayEngine(
            parquet_path,
            callback=on_replay,
            table_callback=table_callback,
            schema=schema,
            batch_size=2,
        ).run()


def test_compare_replay_sorts_by_keys_and_reports_equal() -> None:
    schema = pa.schema(
        [
            ("instrument_id", pa.string()),
            ("dt", pa.int64()),
            ("last_price", pa.float64()),
        ]
    )
    left = pa.table(
        {
            "instrument_id": ["IH2606", "IF2606"],
            "dt": [2, 1],
            "last_price": [2675.0, 3898.0],
        },
        schema=schema,
    )
    right = pa.table(
        {
            "instrument_id": ["IF2606", "IH2606"],
            "dt": [1, 2],
            "last_price": [3898.0, 2675.0],
        },
        schema=schema,
    )

    comparison = zippy.compare_replay(left, right, by=["instrument_id", "dt"])

    assert comparison == {
        "equal": True,
        "left_rows": 2,
        "right_rows": 2,
        "schema_equal": True,
        "missing_rows": 0,
        "extra_rows": 0,
        "mismatch_rows": 0,
    }


def test_reactive_factor_uses_its_own_id_column() -> None:
    schema = pa.schema([("symbol", pa.string()), ("venue", pa.string()), ("price", pa.float64())])
    engine = zippy.NativeProcessor.reactive(
        name="tick_factor_spec_id",
        input_schema=schema,
        id_column="symbol",
        factors=[
            zippy.TsReturnSpec(
                id_column="venue", value_column="price", period=1, output="venue_ret"
            )
        ],
    )
    outputs = []
    try:
        outputs.extend(
            engine.process(
                pa.RecordBatch.from_pydict(
                    {
                        "symbol": ["A", "A", "B"],
                        "venue": ["X", "Y", "X"],
                        "price": [10.0, 100.0, 20.0],
                    },
                    schema=engine.input_schema(),
                )
            )
        )
        received = outputs[-1]
        assert received.column_names == ["symbol", "venue", "price", "venue_ret"]
        assert received.column("venue_ret").to_pylist() == [None, None, 1.0]
    finally:
        engine.close()


def test_python_package_release_version_and_build_revision() -> None:
    pyproject = tomllib.loads((WORKSPACE_ROOT / "pyproject.toml").read_text())
    base_version = pyproject["project"]["version"]
    expected = base_version
    assert zippy.__build_revision__ == git_output("rev-parse", "HEAD")

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


def test_reactive_engine_accepts_all_v1_operators_via_design_helpers() -> None:
    input_schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
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
    engine = zippy.NativeProcessor.reactive(
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
    )
    outputs = []
    assert engine.output_schema() == expected_output_schema
    outputs.extend(
        engine.process(
            pa.RecordBatch.from_pydict(
                {"symbol": ["A", "A", "A"], "price": [10.0, 16.0, 19.0]},
                schema=engine.input_schema(),
            )
        )
    )
    outputs.extend(engine.flush())
    engine.close()


def test_reactive_engine_accepts_expression_factor_helper() -> None:
    input_schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("ema_2", pa.float64(), nullable=False),
            pa.field("price_plus_ema", pa.float64(), nullable=True),
        ]
    )
    engine = zippy.NativeProcessor.reactive(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.TS_EMA(column="price", span=2, output="ema_2"),
            zippy.Expr(expression="price + ema_2", output="price_plus_ema"),
        ],
    )
    outputs = []
    assert engine.output_schema() == expected_output_schema
    outputs.extend(
        engine.process(
            pa.RecordBatch.from_pydict(
                {"symbol": ["A", "A"], "price": [10.0, 16.0]}, schema=engine.input_schema()
            )
        )
    )
    outputs.extend(engine.flush())
    received = outputs[-1]
    assert received.column(2).to_pylist() == [10.0, 14.0]
    assert received.column(3).to_pylist() == [20.0, 30.0]
    engine.close()


def test_reactive_engine_expr_supports_ts_nodes() -> None:
    input_schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    expected_output_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("price", pa.float64()),
            pa.field("score", pa.float64(), nullable=True),
        ]
    )
    engine = zippy.NativeProcessor.reactive(
        name="tick_factors",
        input_schema=input_schema,
        id_column="symbol",
        factors=[
            zippy.Expr(
                expression="TS_DIFF(price, 2) / TS_STD(TS_DIFF(price, 2), 3)", output="score"
            )
        ],
    )
    outputs = []
    assert engine.output_schema() == expected_output_schema
    outputs.extend(
        engine.process(
            pa.RecordBatch.from_pydict(
                {
                    "symbol": ["A", "A", "A", "A", "A", "A"],
                    "price": [10.0, 11.0, 13.0, 16.0, 20.0, 25.0],
                },
                schema=engine.input_schema(),
            )
        )
    )
    outputs.extend(engine.flush())
    received = outputs[-1]
    assert received.column(2).to_pylist() == [
        None,
        None,
        None,
        None,
        pytest.approx(4.286607049870562),
        pytest.approx(5.5113519212621505),
    ]
    engine.close()


def test_expression_factor_rejects_unknown_identifier() -> None:
    input_schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    with pytest.raises(ValueError, match="unknown expression identifier"):
        zippy.NativeProcessor.reactive(
            name="tick_factors",
            input_schema=input_schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="missing + 1.0", output="score")],
        )


def test_expression_factor_rejects_unsupported_function() -> None:
    input_schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    with pytest.raises(ValueError, match="unsupported expression function"):
        zippy.NativeProcessor.reactive(
            name="tick_factors",
            input_schema=input_schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="sqrt(price)", output="score")],
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
    engine = zippy.NativeProcessor.timeseries(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window_ns=int(zippy.Duration.minutes(1)),
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
    )
    outputs = []
    assert engine.output_schema() == expected_output_schema
    outputs.extend(
        engine.process(
            pa.RecordBatch.from_pydict(
                {
                    "symbol": ["A", "A"],
                    "dt": [
                        datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                        datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                    ],
                    "price": [10.0, 11.0],
                    "volume": [100.0, 120.0],
                },
                schema=engine.input_schema(),
            )
        )
    )
    outputs.extend(engine.flush())
    engine.close()


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
    engine = zippy.NativeProcessor.timeseries(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window_ns=int(zippy.Duration.minutes(1)),
        late_data_policy="reject",
        pre_factors=[zippy.Expr(expression="price * volume", output="turnover_input")],
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
        construct_for_rejection(
            zippy.NativeProcessor.timeseries,
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window_ns=int(zippy.Duration.minutes(1)),
            late_data_policy="reject",
            pre_factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            factors=[zippy.AGG_FIRST(column="price", output="open")],
        )
    with pytest.raises(TypeError, match="post_factors"):
        construct_for_rejection(
            zippy.NativeProcessor.timeseries,
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window_ns=int(zippy.Duration.minutes(1)),
            late_data_policy="reject",
            factors=[zippy.AGG_FIRST(column="price", output="open")],
            post_factors=[zippy.AGG_SUM(column="price", output="sum_price")],
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
        zippy.NativeProcessor.timeseries(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window_ns=int(zippy.Duration.minutes(1)),
            late_data_policy="reject",
            factors=[
                zippy.AGG_FIRST(column="price", output="open"),
                zippy.AGG_LAST(column="price", output="close"),
            ],
            post_factors=[zippy.Expr(expression="price * 2.0", output="bad")],
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
        ValueError, match="stateful TS_\\* functions are only supported inside ReactiveStateEngine"
    ):
        zippy.NativeProcessor.timeseries(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window_ns=int(zippy.Duration.minutes(1)),
            late_data_policy="reject",
            pre_factors=[zippy.Expr(expression="TS_DIFF(price, 2)", output="bad")],
            factors=[zippy.AGG_SUM(column="price", output="sum_price")],
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
        ValueError, match="stateful TS_\\* functions are only supported inside ReactiveStateEngine"
    ):
        zippy.NativeProcessor.timeseries(
            name="bar_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window_ns=int(zippy.Duration.minutes(1)),
            late_data_policy="reject",
            factors=[zippy.AGG_SUM(column="price", output="sum_price")],
            post_factors=[zippy.Expr(expression="TS_STD(sum_price, 2)", output="bad")],
        )


def test_timeseries_engine_flush_runs_post_factors() -> None:
    input_schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("dt", pa.timestamp("ns", tz="UTC")),
            ("price", pa.float64()),
            ("volume", pa.float64()),
        ]
    )
    engine = zippy.NativeProcessor.timeseries(
        name="bar_1m",
        input_schema=input_schema,
        id_column="symbol",
        dt_column="dt",
        window_ns=int(zippy.Duration.minutes(1)),
        late_data_policy="reject",
        pre_factors=[zippy.Expr(expression="price * volume", output="turnover_input")],
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
    )
    outputs = []
    outputs.extend(
        engine.process(
            pa.RecordBatch.from_pydict(
                {
                    "symbol": ["A", "A"],
                    "dt": [
                        datetime(2026, 4, 2, 9, 30, 0, tzinfo=timezone.utc),
                        datetime(2026, 4, 2, 9, 30, 1, tzinfo=timezone.utc),
                    ],
                    "price": [10.0, 11.0],
                    "volume": [100.0, 120.0],
                },
                schema=engine.input_schema(),
            )
        )
    )
    outputs.extend(engine.flush())
    received = outputs[-1]
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
    engine.close()


def test_reactive_engine_rejects_invalid_state_failure_policy() -> None:
    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    with pytest.raises(ValueError, match="state_failure_policy"):
        construct_for_rejection(
            zippy.NativeProcessor.reactive,
            name="tick_factors",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.TS_RETURN(column="price", period=1, output="ret_1")],
            state_failure_policy="checkpoint",
        )


def test_reactive_engine_rejects_empty_id_filter() -> None:
    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    with pytest.raises(ValueError, match="id_filter must not be empty"):
        zippy.NativeProcessor.reactive(
            name="reactive_filter",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
            id_filter=[],
        )


@pytest.mark.parametrize("id_filter", [[""], ["  "]])
def test_reactive_engine_rejects_blank_id_filter_values(id_filter) -> None:
    schema = pa.schema([("symbol", pa.string()), ("price", pa.float64())])
    with pytest.raises(ValueError, match="id_filter contains empty value"):
        zippy.NativeProcessor.reactive(
            name="reactive_filter",
            input_schema=schema,
            id_column="symbol",
            factors=[zippy.Expr(expression="price * 2.0", output="price_x2")],
            id_filter=id_filter,
        )


def test_timeseries_engine_rejects_empty_id_filter() -> None:
    input_schema = pa.schema(
        [("symbol", pa.string()), ("dt", pa.timestamp("ns", tz="UTC")), ("price", pa.float64())]
    )
    with pytest.raises(ValueError, match="id_filter must not be empty"):
        zippy.NativeProcessor.timeseries(
            name="bar_filter",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            window_ns=int(zippy.Duration.minutes(1)),
            late_data_policy="reject",
            factors=[zippy.AGG_LAST(column="price", output="close")],
            id_filter=[],
        )


@pytest.mark.parametrize("id_filter", [[""], ["  "]])
def test_timeseries_engine_rejects_blank_id_filter_values(id_filter) -> None:
    schema = pa.schema(
        [("symbol", pa.string()), ("dt", pa.timestamp("ns", tz="UTC")), ("price", pa.float64())]
    )
    with pytest.raises(ValueError, match="id_filter contains empty value"):
        zippy.NativeProcessor.timeseries(
            name="bars",
            input_schema=schema,
            id_column="symbol",
            dt_column="dt",
            late_data_policy="reject",
            factors=[zippy.AGG_LAST(column="price", output="close")],
            window_ns=int(zippy.Duration.minutes(1)),
            id_filter=id_filter,
        )


def test_run_server_daemon_expands_user_path_and_creates_parent_dir(tmp_path: Path) -> None:
    socket_path = tmp_path / ".zippy" / "master.sock"
    script = "import zippy; zippy.run_server_daemon('~/.zippy/master.sock')"
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
                    "run_server_daemon exited before creating expanded socket "
                    f"stdout=[{stdout}] stderr=[{stderr}]"
                )
            time.sleep(0.05)
        else:
            stdout, stderr = process.communicate(timeout=5)
            raise AssertionError(
                "run_server_daemon did not create expanded socket before timeout "
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


def test_cross_sectional_engine_rejects_non_cross_sectional_factor_specs() -> None:
    input_schema = pa.schema(
        [("symbol", pa.string()), ("dt", pa.timestamp("ns", tz="UTC")), ("ret_1m", pa.float64())]
    )
    with pytest.raises(TypeError, match="CSRankSpec|CSZscoreSpec|CSDemeanSpec"):
        construct_for_rejection(
            zippy.NativeProcessor.cross_sectional,
            name="cs_1m",
            input_schema=input_schema,
            id_column="symbol",
            dt_column="dt",
            trigger_interval_ns=int(zippy.Duration.minutes(1)),
            factors=[zippy.AGG_FIRST(column="ret_1m", output="bad")],
        )


def test_row_reuses_exact_dict_values_without_copy() -> None:
    values: dict[str, object] = {
        "instrument_id": "IF2606",
        "last_price": 4102.5,
    }

    row = zippy.Row(values)

    assert row._values is values
    values["last_price"] = 4103.0
    assert row["last_price"] == 4103.0
    assert row.to_dict() == {
        "instrument_id": "IF2606",
        "last_price": 4103.0,
    }


def test_bar_engine_accepts_plugin_profile_protocol() -> None:

    class PluginProfile:

        def to_bar_generator_spec(self) -> dict[str, object]:
            return {
                "frequency": "1m",
                "columns": {
                    "instrument": "instrument_id",
                    "dt": "dt",
                    "price": "last_price",
                    "volume": "volume",
                    "total_turnover": "turnover",
                    "trading_day": "trading_day",
                    "num_trades": None,
                    "limit_up": None,
                    "limit_down": None,
                },
                "sessions": {
                    "timezone": "Asia/Shanghai",
                    "regular": [("09:30:00", "15:00:00")],
                    "auction": [],
                },
                "volume": {"mode": "delta"},
                "auction": "drop",
                "dt_label": "close_dt",
            }

    schema = pa.schema(
        [
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("dt", pa.timestamp("ns", tz="Asia/Shanghai"), nullable=False),
            pa.field("last_price", pa.float64(), nullable=False),
            pa.field("volume", pa.float64(), nullable=False),
            pa.field("turnover", pa.float64(), nullable=False),
            pa.field("trading_day", pa.string(), nullable=False),
        ]
    )
    engine = zippy.NativeProcessor.bars(name="bars", input_schema=schema, profile=PluginProfile())
    assert engine.output_schema().field("volume").type == pa.int64()
    assert engine.output_schema().field("end_dt").type == pa.timestamp("ns", tz="Asia/Shanghai")


def test_setup_log_file_contains_runtime_message(tmp_path: Path) -> None:
    script = """
import json
import sys
import zippy
snapshot = zippy.setup_log("native_test", log_dir=sys.argv[1], to_console=False)
zippy.log_info("native_compute", "completed", "native batch completed", "ok")
print(json.dumps(snapshot))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    path = Path(json.loads(completed.stdout)["file_path"])
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    assert any(
        record.get("component") == "native_compute"
        and record.get("event") == "completed"
        and record.get("message") == "native batch completed"
        and record.get("status") == "ok"
        for record in records
    )
